/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_strnequ.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/02 16:46:30 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/06 21:58:38 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include <string.h>

int		ft_strnequ(char const *s1, char const *s2, size_t n)
{
	char const		*cpy_s1;
	char const		*cpy_s2;

	if (!(cpy_s1 = s1))
		cpy_s1 = "";
	if (!(cpy_s2 = s2))
		cpy_s2 = "";
	while (*cpy_s1 == *cpy_s2 && *cpy_s1 && *cpy_s2 && n)
	{
		cpy_s1++;
		cpy_s2++;
		n--;
	}
	if (!n || *cpy_s1 == *cpy_s2)
		return (1);
	else
		return (0);
}
