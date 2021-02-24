/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_strncat.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/06 17:01:45 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/11 19:16:14 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include <string.h>

char	*ft_strncat(char *s1, const char *s2, size_t n)
{
	int		pos;
	char	*src;

	src = (char *)s2;
	pos = 0;
	while (s1[pos] != '\0')
		pos++;
	while (*src && n)
	{
		s1[pos] = *src;
		pos++;
		src++;
		n--;
	}
	s1[pos] = '\0';
	return (s1);
}
