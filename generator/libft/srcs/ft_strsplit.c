/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_strsplit.c                                      :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/02 19:36:00 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/22 11:09:19 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include <stdlib.h>
#include "libft.h"

static int		ft_len_word(char *str, char c)
{
	int		len;

	len = 0;
	while (str[len])
	{
		if (str[len] == c)
			break ;
		len++;
	}
	return (len);
}

static char		**fill_tab(char **tab, char *str, int words, char c)
{
	int		pos;
	int		word;

	word = 0;
	while (word < words)
	{
		while (*str == c && *str)
			str++;
		pos = -1;
		if (!(tab[word] = (char *)easymalloc(sizeof(**tab) * ft_len_word(str, c)
						+ 1)))
		{
			ft_free_tab((void **)tab, words);
			return (NULL);
		}
		while (*str != c && *str)
			tab[word][++pos] = *(str++);
		tab[word++][++pos] = '\0';
	}
	tab[word] = 0;
	return (tab);
}

char			**ft_strsplit(const char *o_str, char c)
{
	int				words;
	char			**tab;
	const char		*str;

	if (!(str = o_str))
		return (NULL);
	words = 0;
	while (*str)
	{
		if ((*str != c && *(str + 1) == c) || (*str != c && !*(str + 1)))
			words++;
		str++;
	}
	words = c == 0 ? 1 : words;
	str = o_str;
	if (!(tab = (char **)easymalloc(sizeof(char *) * (words + 1))))
		return (NULL);
	return (fill_tab(tab, (char *)str, words, c));
}
