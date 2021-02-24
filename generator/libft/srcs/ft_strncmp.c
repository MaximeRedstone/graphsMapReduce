/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_strncmp.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/05 15:30:16 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/11 19:20:18 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include <string.h>

int		ft_strncmp(const char *s1, const char *s2, size_t n)
{
	unsigned char	*cpy_s1;
	unsigned char	*cpy_s2;

	if (!n)
		return (0);
	cpy_s1 = (unsigned char *)s1;
	cpy_s2 = (unsigned char *)s2;
	while (*cpy_s1 == *cpy_s2 && *cpy_s1 != '\0' && *cpy_s2 != '\0' && n > 1)
	{
		cpy_s1++;
		cpy_s2++;
		n--;
	}
	return (*cpy_s1 - *cpy_s2);
}
